#pragma once
#include <vector>
#include <list>

template<typename T>
class SplitList
{
	uint32_t splitCount;
	uint32_t size;
	std::vector< std::list<T>:iterator > splits;

public:
	std::list<T> list;


	SplitList(int _splitCount)
		:splitCount(_splitCount),
		size(0)
	{
		splits.reserve(50);
	}

	void clear()
	{
		size = 0;
		list.clear();
		splits.clear();
	}

	uint32_t size()
	{
		return size;
	}

	void push_back(auto item)
	{
		list.push_back(item);
		size++;
		if (size % splitCount == 0)
		{
			splits.push_back(container.back());
		}
	}
};